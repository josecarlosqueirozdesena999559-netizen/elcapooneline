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
