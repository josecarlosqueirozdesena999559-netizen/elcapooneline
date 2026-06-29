update robot_settings
set account_mode = 'REAL',
    allow_real = true,
    confirm_real = true
where account_mode is distinct from 'REAL'
   or allow_real is distinct from true
   or confirm_real is distinct from true;

alter table robot_settings alter column account_mode set default 'REAL';
alter table robot_settings alter column allow_real set default true;
alter table robot_settings alter column confirm_real set default true;

update robot_user_settings
set account_mode = 'REAL',
    allow_real = true,
    confirm_real = true
where account_mode is distinct from 'REAL'
   or allow_real is distinct from true
   or confirm_real is distinct from true;

alter table robot_user_settings alter column account_mode set default 'REAL';
alter table robot_user_settings alter column allow_real set default true;
alter table robot_user_settings alter column confirm_real set default true;

update robot_states
set state = jsonb_set(
              jsonb_set(
                jsonb_set(coalesce(state, '{}'::jsonb), '{account_mode}', '"REAL"'::jsonb, true),
                '{allow_real}', 'true'::jsonb, true
              ),
              '{confirm_real}', 'true'::jsonb, true
            ),
    state_json = jsonb_set(
                   jsonb_set(
                     jsonb_set(coalesce(state_json, '{}'::jsonb), '{account_mode}', '"REAL"'::jsonb, true),
                     '{allow_real}', 'true'::jsonb, true
                   ),
                   '{confirm_real}', 'true'::jsonb, true
                 );

update robot_trade_history
set account_mode = 'REAL'
where account_mode is distinct from 'REAL';

alter table robot_trade_history drop constraint if exists robot_trade_history_account_mode_check;
alter table robot_trade_history
  add constraint robot_trade_history_account_mode_check
  check (account_mode = 'REAL');
