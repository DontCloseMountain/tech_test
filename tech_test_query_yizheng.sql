
select 
	date(close_time) as dt_report,
	login_hash as login_hash,
	server_hash as server_hash,
	symbol as symbol,
	currency as currency,
	cast(sum(volume) over(partition by login_hash, server_hash, symbol
					 order by date(close_time)
					 rows between 6 preceding and current row) as float) as sum_volume_prev_7d,
	cast(sum(volume) over(partition by login_hash, server_hash, symbol
					 order by date(close_time)
					 rows between unbounded preceding and current row) as float) as sum_volume_prev_all,
	dense_rank() over(partition by login_hash, symbol
					  order by date(close_time)
					  rows between 6 preceding and current row) as rank_volume_symbol_prev_7d,
	dense_rank() over(partition by login_hash
					  order by date(close_time)
					  rows between 6 preceding and current row) as rank_count_prev_7d,
	sum(volume) filter(where DATE_PART('month', close_time::date) = '08') over(partition by login_hash, server_hash, symbol) as sum_volume_2020_08,
	first_value(date(close_time)) over(partition by login_hash, server_hash, symbol) as date_first_trade,
	row_number() over(order by date(close_time), login_hash, server_hash, symbol) as row_number
from (
  select
    u.login_hash,
    u.server_hash,
    u.currency,
    u.enable,
    t.symbol,
    t.volume,
    t.close_time
  from
    public.trades t 
    inner join public.users u  using (login_hash)
  where
    u.enable = 1
  and DATE_PART('year', close_time::date) = '2020'
  and DATE_PART('month', close_time::date) in ('06', '07', '08', '09')
) as combined_table
order by row_number desc