create table if not exists public.users (
  id text primary key,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

create table if not exists public.bullex_connections (
  id bigint generated always as identity primary key,
  user_id text not null unique references public.users(id) on delete cascade,
  bullex_email text,
  connected boolean not null default false,
  requires_2fa boolean not null default false,
  account_mode text,
  currency text,
  last_balance numeric,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

alter table public.bullex_connections
  add column if not exists last_connected_at timestamptz;

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
  updated_at timestamptz not null default timezone('utc'::text, now()),
  constraint market_assets_user_symbol_key unique (user_id, symbol)
);

create table if not exists public.robot_states (
  user_id text primary key references public.users(id) on delete cascade,
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
  add column if not exists accuracy numeric not null default 0;

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
  updated_at timestamptz not null default timezone('utc'::text, now()),
  constraint robot_trades_user_order_key unique (user_id, order_id)
);

create table if not exists public.robot_restore_status (
  user_id text primary key references public.users(id) on delete cascade,
  session_restored boolean not null default false,
  robot_restored boolean not null default false,
  last_restore_at timestamptz,
  updated_at timestamptz not null default timezone('utc'::text, now())
);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = timezone('utc'::text, now());
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

drop trigger if exists set_robot_restore_status_updated_at on public.robot_restore_status;
create trigger set_robot_restore_status_updated_at
before update on public.robot_restore_status
for each row
execute function public.set_updated_at();
